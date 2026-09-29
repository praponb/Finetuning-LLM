from pyspark.sql import functions as F

result = (
    df_products
    .filter(~F.col("brand").isin(["Acme", "Umbrella"]))
    .groupBy(F.year("launch_date").alias("launch_year"))
    .agg(
        F.count("stock_qty").alias("stock_qty_count"),
        F.sum("cost").alias("total_cost"),
    )
    .orderBy(F.col("stock_qty_count").desc(), "launch_year")
    .limit(5)
)
