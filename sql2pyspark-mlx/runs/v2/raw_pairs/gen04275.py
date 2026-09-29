from pyspark.sql import functions as F

result = (
    df_orders
    .select("status")
    .unionByName(
        df_accounts
        .filter(F.col("opened_date") > "2024-09-12")
        .select("status")
    )
    .orderBy("status")
)
