from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").desc_nulls_first(), "id")
w2 = Window.partitionBy("country").orderBy("country_length", "id")
w3 = Window.partitionBy("country").orderBy("country_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("country").orderBy(F.col("country_length").asc_nulls_first())
w5 = Window.partitionBy("country").orderBy("rating", "id")
w6 = Window.partitionBy("country").orderBy("last_known_rating", "id")

filtered = (
    df_suppliers
    .filter(F.col("city") == "London")
    .select(
        "id",
        "country",
        "rating",
        F.length("country").alias("country_length"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("resolved_city"),
    )
)

ranked = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

not_flagged = (
    ranked
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.product_name").isNotNull()), "left_semi")
)

metrics = (
    not_flagged
    .select(
        "*",
        F.lag("country_length", 1).over(w2).alias("prev_country_length"),
        F.first("rating", ignorenulls=True).over(w3).alias("first_known_rating"),
        F.cume_dist().over(w4).alias("country_length_cume_dist"),
        F.last("rating", ignorenulls=True).over(w3).alias("last_known_rating"),
        F.ntile(3).over(w5).alias("rating_bucket"),
    )
)

scored = (
    metrics
    .select(
        "*",
        F.lag("rating", 1).over(w6).alias("prev_rating"),
        F.row_number().over(w6).alias("row_seq"),
        F.first("first_known_rating", ignorenulls=True).over(w3).alias("first_known_first_known_rating"),
    )
)

result = (
    scored
    .select(
        "id",
        "country",
        F.col("prev_rating").alias("PREV_RATING_VAL"),
        F.col("first_known_rating").alias("FIRST_KNOWN_RATING_VAL"),
        F.col("rating").alias("RATING"),
    )
    .unpivot(
        ids=["id", "country"],
        values=["PREV_RATING_VAL", "FIRST_KNOWN_RATING_VAL", "RATING"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
