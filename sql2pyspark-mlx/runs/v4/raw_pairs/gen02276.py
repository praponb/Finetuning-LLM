from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier", "order_id").orderBy(F.col("cost").desc_nulls_first(), "id")

ranked = (
    df_shipments
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked
    .select(
        "id",
        "delivered_date",
        "cost",
        "status",
        "latest_rank",
        F.when(F.col("status").rlike("^(LOST|RETURNED)$"), "OTHER_ZONE").otherwise("CORE").alias("status_zone"),
    )
    .orderBy(F.col("status").desc_nulls_last(), "status_zone")
)
