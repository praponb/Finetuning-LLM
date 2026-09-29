from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand") == "Initech")
    .select(
        "id",
        "tags",
        F.datediff(F.current_date(), "launch_date").alias("days_since_launch_date"),
        F.array_join("tags", "|").alias("tags_text"),
    )
)
