from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isNotNull())
    .select("status", "customer_id")
    .intersect(
        df_accounts
        .filter(F.col("status").isNotNull())
        .select("status", "customer_id")
    )
    .orderBy("status")
)
