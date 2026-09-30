from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("store_id", "product_id").orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id", "store_id")
w3 = Window.partitionBy(F.month("sale_date")).orderBy("running_max_revenue", "id")
w4 = Window.orderBy("running_max_revenue", "id")
w5 = Window.orderBy("sale_date", "id").rowsBetween(-5, Window.currentRow)
w_ordered = Window.orderBy("prev_running_avg_revenue", "id")

scored = (
    df_store_sales
    .select(
        "id",
        "sale_date",
        F.avg("revenue").over(w_running).alias("running_avg_revenue"),
        F.max("revenue").over(w_running).alias("running_max_revenue"),
        F.sum("revenue").over(w_running).alias("running_sum_revenue"),
        F.min("units").over(w2).alias("product_id_min_units"),
    )
)

metrics = (
    scored
    .select(
        "id",
        "sale_date",
        "running_avg_revenue",
        "running_sum_revenue",
        "product_id_min_units",
        F.lag("running_avg_revenue", 1).over(w3).alias("prev_running_avg_revenue"),
        F.lag("product_id_min_units", 1, 0).over(w4).alias("prev_product_id_min_units"),
        F.avg("product_id_min_units").over(w5).alias("product_id_min_units_avg_5"),
    )
)

enriched = (
    metrics
    .select(
        "id",
        "running_sum_revenue",
        "product_id_min_units",
        "prev_product_id_min_units",
        F.lag("prev_product_id_min_units").over(w_ordered).alias("prev_prev_product_id_min_units"),
        F.lead("prev_product_id_min_units").over(w_ordered).alias("next_prev_product_id_min_units"),
        F.lead("product_id_min_units_avg_5").over(w_ordered).alias("next_product_id_min_units_avg_5"),
        F.row_number().over(w_ordered).alias("seq_no"),
    )
)

base = (
    enriched
    .filter(F.col("product_id_min_units") <= 40)
    .select(
        "id",
        "running_sum_revenue",
        "product_id_min_units",
        "prev_prev_product_id_min_units",
        "next_product_id_min_units_avg_5",
        (F.col("next_prev_product_id_min_units") - F.col("running_sum_revenue")).alias("next_prev_product_id_min_units_minus_running_sum_revenue"),
    )
)

result = (
    base
    .select(
        "id",
        "running_sum_revenue",
        "product_id_min_units",
        "next_product_id_min_units_avg_5",
        "next_prev_product_id_min_units_minus_running_sum_revenue",
    )
    .orderBy("product_id_min_units", F.col("id").desc())
    .limit(10)
)
