from pyspark.sql import functions as F

result = (
    df_accounts
    .select("status")
    .unionByName(
        df_payments
        .filter(F.to_date("paid_at") < "2023-03-01")
        .select("status")
    )
    .distinct()
)
