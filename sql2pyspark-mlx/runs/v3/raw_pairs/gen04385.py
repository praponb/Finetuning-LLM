from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(~F.col("account_type").isin(["SAVINGS", "CREDIT", "CHECKING"]))
    .select("opened_date")
    .union(df_products.select("launch_date"))
    .distinct()
    .orderBy("opened_date")
)
