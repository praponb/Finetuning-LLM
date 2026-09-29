from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("name").like("Al%"))
    .select("signup_date")
    .union(
        df_inventory
        .filter(F.col("product_id").isin([1, 2, 6, 9]))
        .select("last_restock")
    )
    .distinct()
)
