from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("cost").desc_nulls_last())
w2 = Window.partitionBy(F.month("shipped_date"), "status").orderBy("delivered_date", "id")

cleaned = (
    df_shipments
    .filter((F.col("cost") < 68) & F.col("carrier").isNotNull())
    .withColumn("is_kerry", F.when(F.col("carrier") == "Kerry", 1).otherwise(0))
)

result = (
    cleaned
    .select(
        "id",
        "order_id",
        "carrier",
        "delivered_date",
        "cost",
        "status",
        "is_kerry",
        F.cume_dist().over(w).alias("cost_cume_dist"),
        F.lead("cost", 2).over(w2).alias("next_cost"),
    )
)
