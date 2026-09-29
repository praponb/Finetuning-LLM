from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .filter(~F.col("p.category").isin(["Grocery", "Sports", "Electronics"]) & F.col("p.category").isin(["Books", "Grocery", "Electronics"]))
    .select(
        F.col("ss.id"),
        F.col("ss.sale_date"),
        F.col("p.price"),
        F.col("p.product_name"),
        F.col("p.brand"),
    )
    .orderBy("id")
    .limit(20)
)
