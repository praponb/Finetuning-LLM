from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("amount").asc_nulls_last(), "id")
cost_by_order_id = df_shipments.groupBy("order_id").agg(F.min("cost").alias("min_shipment_cost"))

valid = (
    df_shipments
    .filter(F.col("delivered_date") >= F.lit("2023-04-28").cast("date"))
    .select(
        "id",
        "order_id",
        "shipped_date",
        "delivered_date",
        F.when(F.col("cost") >= 95, "High").when(F.col("cost") >= 14, "Medium").otherwise("Low").alias("cost_level"),
    )
)

joined = (
    valid
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id").eqNullSafe(F.col("b.id")), "left")
    .select(
        "src.id",
        "src.order_id",
        "src.shipped_date",
        "src.cost_level",
        F.col("b.discount"),
        F.col("b.order_date"),
        F.col("b.amount"),
    )
)

deduped = (
    joined
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

enriched = (
    deduped
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "inner")
    .select(
        "a.id",
        "a.order_id",
        "a.shipped_date",
        "a.cost_level",
        "a.amount",
        "a.row_num",
        F.col("d.status"),
        F.col("d.order_date").alias("order_order_date"),
    )
)

result = (
    enriched
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "shipped_date",
        "cost_level",
        "amount",
        "status",
        "order_order_date",
        F.when(F.col("status").rlike("^(CANCELLED|COMPLETED)$"), "GROUP_B").when(F.col("status").rlike("^(PENDING|SHIPPED)$"), "CORE").otherwise("GROUP_A").alias("status_zone"),
        F.col("min_shipment_cost"),
    )
    .orderBy(F.col("id").desc_nulls_last(), "order_id")
)
