from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("shipping_fee_plus_discount").asc_nulls_first(), "id")
w2 = Window.partitionBy("store_id", "channel").orderBy("opened_date", "id")
w3 = Window.orderBy("shipping_fee_plus_discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
units_by_store_id = df_store_sales.groupBy("store_id").agg(F.avg("units").alias("avg_store_sale_units"))

base = (
    df_orders
    .filter((~F.col("channel").eqNullSafe(F.lit("web"))) & (F.col("order_date") >= F.to_date(F.lit("2023-05-19"))))
    .select(
        "id",
        "store_id",
        "status",
        "channel",
        "shipping_fee",
        (F.col("shipping_fee") + F.col("discount")).alias("shipping_fee_plus_discount"),
        F.year("order_date").alias("order_date_year"),
    )
)

deduped = (
    base
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

with_dims = (
    deduped
    .alias("src")
    .join(df_stores.alias("b"), F.col("src.store_id").eqNullSafe(F.col("b.id")), "inner")
    .filter(~F.col("b.city").isin(["Berlin", "New York", "Tokyo"]))
    .select("src.*", F.col("b.opened_date"))
)

enriched = (
    with_dims
    .select(
        "*",
        F.lead("shipping_fee_plus_discount").over(w2).alias("next_shipping_fee_plus_discount"),
        F.lag("shipping_fee_plus_discount").over(w2).alias("prev_shipping_fee_plus_discount"),
        F.lag("order_date_year").over(w2).alias("prev_order_date_year"),
        F.lead("order_date_year").over(w2).alias("next_order_date_year"),
        F.row_number().over(w2).alias("row_seq"),
        F.first("shipping_fee", ignorenulls=True).over(w3).alias("first_known_shipping_fee"),
    )
)

result = (
    enriched
    .join(units_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "shipping_fee",
        "shipping_fee_plus_discount",
        "order_date_year",
        "rn",
        "opened_date",
        "next_shipping_fee_plus_discount",
        "prev_shipping_fee_plus_discount",
        F.col("avg_store_sale_units"),
    )
    .orderBy("prev_shipping_fee_plus_discount")
)
