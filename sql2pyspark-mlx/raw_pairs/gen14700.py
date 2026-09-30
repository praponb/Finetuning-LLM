from pyspark.sql import functions as F

result = (
    df_products
    .select("category")
    .subtract(
        df_orders
        .filter(F.col("channel").isin(["store", "mobile"]))
        .select("status")
    )
    .orderBy("category")
)
