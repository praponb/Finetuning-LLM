from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") >= 3400)
    .select("sale_date")
    .subtract(
        df_subscriptions
        .filter(~F.col("plan").isin(["basic", "standard"]))
        .select("end_date")
    )
)
