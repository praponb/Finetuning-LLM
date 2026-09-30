from pyspark.sql import functions as F

result = (
    df_customers
    .select("signup_date")
    .union(
        df_inventory
        .filter(F.col("quantity").isNotNull())
        .select("last_restock")
    )
    .distinct()
    .orderBy("signup_date")
)
