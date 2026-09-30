from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("sale_date"), "store_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id", "product_id")
w3 = Window.partitionBy("product_id").orderBy(F.col("revenue").desc(), "id")
w4 = Window.partitionBy("product_id").orderBy("corr_revenue_units", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.partitionBy("store_id")
w6 = Window.orderBy(F.col("seq_no").desc_nulls_first())
w7 = Window.orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy(F.month("sale_date_month_end")).orderBy("corr_revenue_units")
w9 = Window.orderBy("sale_date_month_end", "id").rowsBetween(-6, Window.currentRow)
w10 = Window.orderBy("revenue_avg_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_store_sales
    .select(
        "*",
        F.sum("revenue").over(w).alias("running_revenue"),
        F.corr("revenue", "units").over(w2).alias("corr_revenue_units"),
        F.row_number().over(w3).alias("seq_no"),
    )
)

enriched = (
    scored
    .select(
        "id",
        "sale_date",
        "units",
        "revenue",
        "corr_revenue_units",
        F.avg("revenue").over(w4).alias("revenue_avg_2"),
        F.sum("seq_no").over(w5).alias("store_id_sum_seq_no"),
        F.rank().over(w6).alias("seq_no_rank"),
    )
)

filtered = (
    enriched
    .filter(F.col("sale_date") > F.lit("2023-04-30").cast("date"))
    .select(
        "id",
        "units",
        "revenue",
        "corr_revenue_units",
        "revenue_avg_2",
        "store_id_sum_seq_no",
        F.last_day("sale_date").alias("sale_date_month_end"),
    )
)

enriched_store_sales = (
    filtered
    .select(
        "id",
        "revenue_avg_2",
        "sale_date_month_end",
        F.first("store_id_sum_seq_no", ignorenulls=True).over(w7).alias("first_known_store_id_sum_seq_no"),
        F.percent_rank().over(w8).alias("corr_revenue_units_percent_rank"),
        F.avg("store_id_sum_seq_no").over(w9).alias("store_id_sum_seq_no_avg_6"),
        F.last("store_id_sum_seq_no", ignorenulls=True).over(w10).alias("last_known_store_id_sum_seq_no"),
    )
)

result = (
    enriched_store_sales
    .groupBy("first_known_store_id_sum_seq_no")
    .agg(
        F.sum("last_known_store_id_sum_seq_no").alias("total_last_known_store_id_sum_seq_no"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.percentile_approx("last_known_store_id_sum_seq_no", [0.25, 0.5, 0.75]).alias("last_known_store_id_sum_seq_no_quartiles"),
        F.round(F.avg("first_known_store_id_sum_seq_no"), 2).alias("avg_first_known_store_id_sum_seq_no"),
    )
    .orderBy(F.col("distinct_ids").asc_nulls_first())
)
