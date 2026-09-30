from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("shipped_date", "id").rowsBetween(-6, -1)
w2 = Window.orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("order_id", "carrier")

cleaned = (
    df_shipments
    .filter(F.col("order_id").isNotNull())
    .select("*", F.col("cost").cast("int").alias("cost_int"))
)

result = (
    cleaned
    .select(
        "*",
        F.avg("cost").over(w).alias("cost_avg_6_prior"),
        F.first("cost_int", ignorenulls=True).over(w2).alias("first_known_cost_int"),
        F.count("cost_int").over(w3).alias("order_id_count_cost_int"),
    )
)
