from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("price") <= 840) | (F.col("brand") == "Globex"))
    .select(
        "tags",
        "supplier_id",
        F.array_join("tags", "|").alias("tags_text"),
        F.date_trunc("WEEK", "launch_date").alias("launch_date_period"),
    )
    .orderBy(F.col("supplier_id").desc())
)
