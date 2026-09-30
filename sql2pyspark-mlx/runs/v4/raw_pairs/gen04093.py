from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("stock_qty").desc(), "id")

base = (
    df_products
    .filter((F.col("launch_date") > F.lit("2024-02-28").cast("date")) & F.col("brand").isNotNull())
    .select(
        "id",
        "product_name",
        "brand",
        "stock_qty",
        "launch_date",
        "tags",
        F.array_contains("tags", "premium").alias("is_premium"),
        F.coalesce("brand", F.lit("OTHER")).alias("resolved_brand"),
    )
)

result = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)
