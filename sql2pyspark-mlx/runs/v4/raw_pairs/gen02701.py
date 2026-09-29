from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status") != "RETURNED")
    .select("amount")
    .intersect(df_products.filter(F.col("supplier_id").isNotNull()).select("price"))
    .orderBy("amount")
)
