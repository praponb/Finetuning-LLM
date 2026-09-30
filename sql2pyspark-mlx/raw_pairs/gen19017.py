from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("revenue").asc_nulls_first(), "id")
w2 = Window.partitionBy("product_id").orderBy("revenue_per_unit", "id")
w_part = Window.orderBy("sale_date", "id")
w_ordered = Window.orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.date_trunc("week", "sale_date"))
w_running = Window.orderBy("next_revenue", "id")

ranked = (
    df_store_sales
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

filtered = (
    ranked
    .filter(F.col("units") <= 12)
    .withColumn("revenue_per_unit", (F.col("units") - F.col("revenue")))
)

ranked_store_sales = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
    .select("id", "sale_date", "units", "revenue", "revenue_per_unit")
)

enriched = (
    ranked_store_sales
    .filter(F.col("revenue") > 3500)
    .select(
        "id",
        "sale_date",
        "units",
        F.lag("units").over(w_part).alias("prev_units"),
        F.lead("units").over(w_part).alias("next_units"),
        F.lead("revenue").over(w_part).alias("next_revenue"),
        F.row_number().over(w_part).alias("row_seq"),
        F.first("revenue_per_unit", ignorenulls=True).over(w_ordered).alias("first_known_revenue_per_unit"),
    )
)

scored = (
    enriched
    .select(
        "id",
        "sale_date",
        "units",
        "prev_units",
        "next_units",
        "next_revenue",
        F.max("next_revenue").over(w5).alias("sale_date_period_max_next_revenue"),
        F.lead("next_revenue", 2).over(w_running).alias("next_next_revenue"),
        F.lag("row_seq", 1).over(w_running).alias("prev_row_seq"),
        F.lead("units", 2).over(w_part).alias("next_units_2"),
    )
)

result = (
    scored
    .select(
        "id",
        "units",
        "prev_units",
        "sale_date_period_max_next_revenue",
        "next_next_revenue",
    )
)
