from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "status").orderBy(F.col("start_date").asc_nulls_first(), "id")

valid = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull() & (F.col("status") != "CANCELLED"))
    .select(
        "id",
        "customer_id",
        "start_date",
        "status",
        F.date_add("start_date", 5).alias("start_date_plus_5d"),
    )
)

result = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)
