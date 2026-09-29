from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price") <= 860)
    .groupBy(F.date_trunc("MONTH", "launch_date").alias("launch_month"))
    .agg(
        F.sum(F.when(F.col("category") == "Grocery", F.col("cost")).otherwise(0)).alias("grocery_cost"),
    )
    .orderBy(F.col("grocery_cost").desc())
)
