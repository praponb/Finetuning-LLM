from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("shipped_date").desc(), "id")
w2 = Window.partitionBy("status").orderBy("delivered_date", "id")
w3 = Window.partitionBy("status").orderBy("shipped_date", "id")

base = (
    df_shipments
    .filter((F.col("delivered_date") >= F.to_date(F.lit("2023-06-24"))) & (F.col("carrier").eqNullSafe(F.lit("Flash"))))
    .select(
        "id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "status",
        F.concat("status", F.lit(" "), "carrier").alias("status_carrier"),
        F.coalesce("carrier", F.lit("DIRECT")).alias("resolved_carrier"),
    )
)

result = (
    base
    .filter(F.year("delivered_date") >= 2025)
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("row_seq", F.row_number().over(w3))
)
