from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "id",
        "brand",
        "category",
        "launch_date",
        F.round("cost", 0).alias("cost_rounded"),
    )
    .orderBy("id", F.col("category").desc())
)
