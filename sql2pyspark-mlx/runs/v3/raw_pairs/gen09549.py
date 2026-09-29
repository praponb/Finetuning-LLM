from pyspark.sql import functions as F

result = (
    df_products
    .select("price")
    .intersect(
        df_transactions
        .filter(F.col("txn_type").isin(["CREDIT", "REFUND", "FEE"]))
        .select("amount")
    )
)
