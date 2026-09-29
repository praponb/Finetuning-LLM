from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.array_contains("tags", "new") & (F.col("launch_date") < "2023-07-24"))
    .select(
        "id",
        "price",
        "cost",
        "category",
        F.date_format("launch_date", "yyyy-MM-dd").alias("launch_date_label"),
        (F.datediff(F.current_date(), "launch_date") / 365).alias("years_since_launch_date"),
    )
    .orderBy("launch_date_label")
)
