from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city").orderBy("rating", "id")

cleaned = (
    df_suppliers
    .filter((~F.col("city").eqNullSafe(F.lit("Singapore"))) & F.col("supplier_name").like("Si%"))
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.lower("country").alias("country_lower"),
    )
)

eligible = (
    cleaned
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.cost") < 550), "left_semi")
)

enriched = (
    eligible
    .select(
        "*",
        F.sum("rating").over(w).alias("running_sum_rating"),
        F.min("rating").over(w).alias("running_min_rating"),
        F.avg("rating").over(w).alias("running_avg_rating"),
        F.max("rating").over(w).alias("running_max_rating"),
        F.ntile(5).over(w2).alias("rating_bucket"),
    )
)

valid = (
    enriched
    .filter(F.col("city").isNotNull() & (F.col("rating_bucket") != 5))
    .select(
        "id",
        "supplier_name",
        "city",
        "rating",
        "country_lower",
        "running_min_rating",
        "running_max_rating",
        "rating_bucket",
        F.lower("city").alias("city_lower"),
    )
)

result = (
    valid
    .select("id", "city", "rating", "running_max_rating", "running_min_rating")
    .unpivot(
        ids=["id", "city"],
        values=["rating", "running_max_rating", "running_min_rating"],
        variableColumnName="measure",
        valueColumnName="val",
    )
)
