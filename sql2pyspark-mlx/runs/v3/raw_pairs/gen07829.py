from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity").between(144, 198))
    .select("last_restock")
    .union(df_customers.select("signup_date"))
)
