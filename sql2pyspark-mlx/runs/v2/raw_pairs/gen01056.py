from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand", F.date_trunc("MONTH", "launch_date").alias("launch_month"))
    .agg(
        F.sum(F.when(F.col("category") == "Grocery", F.col("price")).otherwise(0)).alias("grocery_price"),
        F.sum("cost").alias("total_cost"),
    )
    .filter(F.col("total_cost") < 4100)
    .drop("total_cost")
)
