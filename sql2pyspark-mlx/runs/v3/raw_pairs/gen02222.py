from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand").isin(["Acme", "Initech"]) & ((F.col("category") == "Fashion") | F.col("category").isin(["Fashion", "Sports", "Grocery"])))
    .groupBy(
        "supplier_id",
        F.when(F.col("cost") >= 650, "high").when(F.col("cost") >= 130, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(F.sum("stock_qty").alias("total_stock_qty"))
    .filter(F.col("total_stock_qty") < 1100)
    .orderBy(
        F.col("total_stock_qty").desc(),
        F.col("supplier_id").desc(),
        "cost_bucket",
    )
    .limit(10)
)
