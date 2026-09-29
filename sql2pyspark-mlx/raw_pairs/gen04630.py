from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("amount_with_shipping_avg_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status").orderBy(F.col("shipping_fee").desc(), "id")

valid = (
    df_orders
    .filter((~F.col("channel").eqNullSafe(F.lit("store"))) & (F.col("order_date") >= F.lit("2023-10-05").cast("date")))
    .select(
        "id",
        "store_id",
        "order_date",
        "status",
        "amount",
        "discount",
        "shipping_fee",
        (F.col("shipping_fee") + F.col("amount")).alias("amount_with_shipping"),
        F.col("amount").cast("int").alias("amount_int"),
    )
)

enriched = (
    valid
    .alias("s")
    .join(df_stores.alias("d"), F.col("s.store_id").eqNullSafe(F.col("d.id")), "left")
    .select("s.*", F.col("d.city"), F.col("d.store_name"), F.col("d.opened_date"))
)

scored = (
    enriched
    .filter(F.col("order_date") < "2024-03-01")
    .withColumn("cumulative_amount_int", F.sum("amount_int").over(w))
    .withColumn("amount_with_shipping_avg_2", F.avg("amount_with_shipping").over(w_part))
)

scored_orders = (
    scored
    .select(
        "id",
        "status",
        "discount",
        "shipping_fee",
        "amount_with_shipping",
        "amount_int",
        "opened_date",
        "amount_with_shipping_avg_2",
        F.sum("amount_int").over(w3).alias("running_sum_amount_int"),
        F.max("amount_int").over(w3).alias("running_max_amount_int"),
        F.avg("amount_int").over(w3).alias("running_avg_amount_int"),
    )
)

ranked = (
    scored_orders
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "status",
        "discount",
        "shipping_fee",
        "opened_date",
        "amount_with_shipping_avg_2",
        "running_sum_amount_int",
        "running_max_amount_int",
    )
)

result = (
    ranked
    .select(
        "id",
        "discount",
        "shipping_fee",
        "opened_date",
        "amount_with_shipping_avg_2",
        "running_sum_amount_int",
        "running_max_amount_int",
    )
)
