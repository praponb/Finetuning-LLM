from pyspark.sql import functions as F

result = (
    df_accounts
    .select("customer_id", "id")
    .intersect(
        df_accounts
        .filter(F.col("balance").between(30000, 55000))
        .select("customer_id", "id")
    )
)
