from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date").between("2025-03-01", "2025-08-28"))
    .select("shipped_date")
    .subtract(
        df_accounts
        .filter(~F.col("account_type").isin(["SAVINGS", "LOAN"]))
        .select("opened_date")
    )
)
