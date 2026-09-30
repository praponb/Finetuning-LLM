from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "launch_date",
        "category",
        "cost",
        F.array_join("tags", ", ").alias("tags_text"),
    )
    .orderBy(F.col("launch_date").desc(), F.col("category").desc())
)
