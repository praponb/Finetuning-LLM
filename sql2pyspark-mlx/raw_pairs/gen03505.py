from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("rating").desc_nulls_last())
w3 = Window.partitionBy("country").orderBy("rating", "id")
w4 = Window.orderBy("rating", "id").rowsBetween(-5, -1)
w5 = Window.orderBy(F.col("rating_cume_dist").desc())
w_recent = Window.partitionBy("country").orderBy("rating_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("country", "city").orderBy("rating", "id")

scored = (
    df_suppliers
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.sum("rating").over(w).alias("cumulative_rating"),
        F.cume_dist().over(w2).alias("rating_cume_dist"),
    )
)

not_flagged = (
    scored
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.array_contains(F.col("x.tags"), "eco")), "left_anti")
)

scored_suppliers = (
    not_flagged
    .select(
        "id",
        "city",
        "rating",
        F.lag("rating_cume_dist", 1).over(w3).alias("prev_rating_cume_dist"),
        F.avg("cumulative_rating").over(w4).alias("cumulative_rating_avg_5_prior"),
        F.percent_rank().over(w5).alias("rating_cume_dist_percent_rank"),
        F.last("rating", ignorenulls=True).over(w_recent).alias("last_known_rating"),
        F.row_number().over(w7).alias("row_seq"),
    )
)

remaining_keys = (
    scored_suppliers
    .select("city")
    .intersect(
        df_suppliers
        .filter(F.lower(F.trim("supplier_name")) == "siam parts co")
        .select("city")
    )
)

verified = (
    scored_suppliers
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "id",
        "city",
        "rating",
        "rating_cume_dist_percent_rank",
        "last_known_rating",
        F.when(F.col("city").rlike("^(New York|Berlin)$"), "PRIORITY").otherwise("GROUP_A").alias("city_zone"),
    )
)
