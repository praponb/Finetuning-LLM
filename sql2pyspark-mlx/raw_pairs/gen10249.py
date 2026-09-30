from pyspark.sql import functions as F

result = (
    df_orders
    .filter(~F.col("channel").isin(["web", "store", "mobile"]))
    .select(
        "id",
        "status",
        "discount",
        F.when(F.col("channel") == "web", 1).otherwise(0).alias("is_web"),
    )
    .orderBy("id")
    .limit(10)
)
