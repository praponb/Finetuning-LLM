from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("shipped_date", "id")

ranked = (
    df_shipments
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked
    .select(
        "id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        "latest_rank",
        F.when(F.col("carrier").rlike("^(UPS|Kerry)$"), "CORE").otherwise("EXTENDED").alias("carrier_zone"),
    )
    .orderBy(F.col("carrier_zone").desc_nulls_first(), F.col("id").desc())
)
