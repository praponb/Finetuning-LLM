from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("last_restock_period", "id")
w2 = Window.partitionBy("product_id").orderBy("last_restock_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_inventory
    .filter(F.col("last_restock") >= F.to_date(F.lit("2023-03-31")))
    .withColumn("quantity_filled", F.coalesce("quantity", F.lit(0)))
)

base = (
    cleaned
    .filter(F.col("quantity_filled").between(36, 59))
    .select(
        "id",
        "product_id",
        "last_restock",
        F.add_months("last_restock", 3).alias("last_restock_plus_months"),
        F.date_trunc("year", "last_restock").alias("last_restock_period"),
    )
)

with_dims = (
    base
    .alias("a")
    .join(df_products.alias("d"), F.col("a.product_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.product_name"))
)

with_prev = (
    with_dims
    .withColumn("prev_last_restock_period", F.lag("last_restock_period").over(w))
)

with_gaps = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_last_restock_period").isNull() | (F.col("last_restock_period") > F.col("prev_last_restock_period") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "product_id",
        "last_restock",
        "last_restock_period",
        F.sum("is_new_session").over(w2).alias("session_no"),
    )
)

result = (
    islands
    .groupBy(F.date_trunc("year", "last_restock").alias("year_start"))
    .agg(
        F.max("session_no").alias("max_session_no"),
        F.min("last_restock_period").alias("first_last_restock_period"),
        ((F.unix_timestamp(F.max("last_restock_period")) - F.unix_timestamp(F.min("last_restock_period"))) / 60).alias("duration_min"),
    )
    .filter(F.col("duration_min") > 0)
    .orderBy(F.col("max_session_no").desc_nulls_last())
)
