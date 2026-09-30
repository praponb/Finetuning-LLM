from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category").isin(["Home", "Sports"]) & (F.col("category") == "Fashion"))
    .select(
        "category",
        "tags",
        "cost",
        F.datediff(F.current_date(), "launch_date").alias("days_since_launch_date"),
        F.weekofyear("launch_date").alias("launch_date_weekofyear"),
    )
)
