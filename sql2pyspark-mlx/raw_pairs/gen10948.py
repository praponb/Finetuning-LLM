from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("delivered_date", "id").rowsBetween(-6, Window.currentRow)

valid = (
    df_shipments
    .filter(F.col("order_id").isNotNull() & F.month("delivered_date").isin([6, 7, 12]))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        F.round((F.col("cost") * 100), 2).alias("cost_scaled"),
        F.coalesce("carrier", F.lit("OTHER")).alias("resolved_carrier"),
    )
)

result = (
    valid
    .select(
        "id",
        "shipped_date",
        "delivered_date",
        "cost",
        "cost_scaled",
        F.sum("cost_scaled").over(w).alias("last7_sum_cost_scaled"),
        F.count("cost_scaled").over(w).alias("last7_count_cost_scaled"),
        F.max("cost_scaled").over(w).alias("last7_max_cost_scaled"),
    )
)
