from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(((F.year("opened_date") == 2024) & (F.col("balance") >= 66000)) | (F.col("status") == "OPEN"))
    .select("balance", "opened_date", "status")
    .orderBy(F.col("opened_date").desc(), "balance")
)
