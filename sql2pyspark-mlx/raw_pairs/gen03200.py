from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "order_id").orderBy(F.col("delivered_date").desc())

valid = (
    df_shipments
    .filter(F.col("delivered_date") < "2024-10-01")
    .select(
        "id",
        "order_id",
        "carrier",
        "delivered_date",
        "cost",
        "status",
        F.add_months("shipped_date", 6).alias("shipped_date_plus_months"),
    )
)

result = (
    valid
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "delivered_date", "status", "shipped_date_plus_months")
)
