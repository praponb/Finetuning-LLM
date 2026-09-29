from pyspark.sql import functions as F

result = (
    df_shipments
    .select("shipped_date")
    .subtract(df_accounts.filter(F.col("status") != "CLOSED").select("opened_date"))
)
