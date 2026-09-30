from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy(F.col("quantity").asc_nulls_last(), "id")
w_running = Window.partitionBy("product_id").orderBy("opened_date", "id")
w3 = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("city").orderBy("launch_date", "id")
w5 = Window.partitionBy("city").orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("region").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy(F.date_trunc("month", "last_restock"), "region").orderBy("last_restock_month_end", "id").rowsBetween(-6, Window.currentRow)

valid = (
    df_inventory
    .filter(F.col("quantity") != 101)
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.last_day("last_restock").alias("last_restock_month_end"),
    )
)

enriched = (
    valid
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.product_name").like("%Maker%"))
    .select("s.*", F.col("p.product_name"), F.col("p.launch_date"), F.col("p.cost"))
)

deduped = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

enriched_inventory = (
    deduped
    .alias("e")
    .join(df_stores.alias("d"), F.col("e.store_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.opened_date"), F.col("d.region"), F.col("d.city"))
)

metrics = (
    enriched_inventory
    .select(
        "id",
        "last_restock",
        "last_restock_month_end",
        "launch_date",
        "cost",
        "dedup_rank",
        "opened_date",
        "region",
        "city",
        F.lead("quantity").over(w_running).alias("next_quantity"),
        F.lead("cost").over(w_running).alias("next_cost"),
        F.row_number().over(w_running).alias("seq_no"),
        F.first("quantity", ignorenulls=True).over(w3).alias("first_known_quantity"),
    )
)

latest = (
    metrics
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
)

result = (
    latest
    .select(
        "id",
        "last_restock_month_end",
        "launch_date",
        "dedup_rank",
        "next_quantity",
        "next_cost",
        "seq_no",
        "row_num",
        F.sum("next_quantity").over(w5).alias("running_next_quantity"),
        F.sum("cost").over(w6).alias("cumulative_cost"),
        F.avg("cost").over(w7).alias("cost_avg_6"),
    )
)
