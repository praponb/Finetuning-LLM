from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(~F.col("account_type").isin(["LOAN", "CHECKING", "CREDIT"]))
    .select("opened_date")
    .union(df_store_sales.select("sale_date"))
    .distinct()
)
