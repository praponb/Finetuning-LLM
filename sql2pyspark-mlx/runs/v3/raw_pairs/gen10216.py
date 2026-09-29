from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date") <= "2023-05-02")
    .select("status")
    .unionByName(df_payments.filter(F.col("method") == "card").select("status"))
)
