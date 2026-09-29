from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_end_date_year = Window.partitionBy("product", F.year("end_date"))
w_all_by_end_date_ties = Window.orderBy("end_date")
w_product_by_start_date_span_d = Window.partitionBy("product").orderBy(F.col("start_date_span_days").desc(), "product", "start_date_ym", "end_date_year")
w_all_by_start_date_span_days = Window.orderBy(F.col("start_date_span_days").desc(), "product", "start_date_ym", "end_date_year")
w_product_distinct_products = Window.partitionBy("product", "distinct_products")
w_all_by_start_date_span_days_2 = Window.orderBy("start_date_span_days", "product", "start_date_ym", "end_date_year")
w_distinct_products_by_rows_in = Window.partitionBy("distinct_products").orderBy(F.col("rows_in_product_distinct_products").desc(), "product", "start_date_ym", "end_date_year").rowsBetween(-1, Window.currentRow)
w_distinct_products_by_rows_in_2 = Window.partitionBy("distinct_products").orderBy(F.col("rows_in_product_distinct_products").desc(), "product", "start_date_ym", "end_date_year")
w_distinct_products_by_rows_in_3 = Window.partitionBy("distinct_products").orderBy(F.col("rows_in_product_distinct_products").desc(), "product", "start_date_ym", "end_date_year").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_distinct_products_by_rows_in_4 = Window.partitionBy("distinct_products").orderBy(F.col("rows_in_product_distinct_products").desc(), "product", "start_date_ym", "end_date_year").rowsBetween(-6, Window.currentRow)

scoped = (
    df_policies
    .filter((F.col("end_date").isNotNull()) & (F.col("premium") >= 2096.6))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
    )
)

holder_id_stats = (
    scoped
    .groupBy("holder_id")
    .agg(F.avg("premium").alias("avg_premium"))
)

vs_group = (
    scoped.alias("s")
    .join(holder_id_stats.alias("g"), F.col("g.holder_id") == F.col("s.holder_id"), "inner")
    .filter(F.col("s.premium") > F.col("g.avg_premium"))
    .select(
        "s.*",
        "g.avg_premium",
        F.round(F.col("s.premium") - F.col("g.avg_premium"), 2).alias("premium_minus_avg"),
    )
)

sequenced = (
    vs_group
    .select(
        "id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "avg_premium",
        F.count(F.lit(1)).over(w_product_end_date_year).alias("rows_in_product_end_date_year"),
        F.round(F.cume_dist().over(w_all_by_end_date_ties), 3).alias("end_date_cume_dist"),
    )
)

summary = (
    sequenced
    .groupBy(
        "product",
        F.date_format("start_date", "yyyy-MM").alias("start_date_ym"),
        F.year("end_date").alias("end_date_year"),
    )
    .agg(
        F.array_sort(F.collect_set("product")).alias("product_list"),
        F.countDistinct("product").alias("distinct_products"),
        F.round(F.sum("end_date_cume_dist") / F.nullif(F.sum("rows_in_product_end_date_year"), F.lit(0)), 3).alias("end_date_cume_dist_to_rows_in_product_end_date_y"),
        F.datediff(F.max("start_date"), F.min("start_date")).alias("start_date_span_days"),
        F.round(F.sum("avg_premium") / F.nullif(F.sum("rows_in_product_end_date_year"), F.lit(0)), 3).alias("avg_premium_to_rows_in_product_end_date_year_rat"),
        F.sum("end_date_cume_dist").alias("sum_end_date_cume_dist_check"),
    )
    .filter(F.col("sum_end_date_cume_dist_check") > 0.1)
    .drop("sum_end_date_cume_dist_check")
)

windowed = (
    summary
    .select(
        "*",
        F.row_number().over(w_product_by_start_date_span_d).alias("rn"),
        F.lag("start_date_span_days", 3).over(w_all_by_start_date_span_days).alias("prev3_start_date_span_days"),
        F.count(F.lit(1)).over(w_product_distinct_products).alias("rows_in_product_distinct_products"),
        F.ntile(10).over(w_all_by_start_date_span_days_2).alias("start_date_span_days_decile"),
        F.round(F.col("distinct_products") / F.nullif(F.sum("distinct_products").over(w_product_distinct_products), F.lit(0)), 4).alias("distinct_products_share_of_product_distinct_prod"),
    )
)

with_trends = (
    windowed
    .select(
        "*",
        F.round(F.avg("prev3_start_date_span_days").over(w_distinct_products_by_rows_in), 2).alias("prev3_start_date_span_days_avg_2"),
        F.lead("prev3_start_date_span_days", 3).over(w_distinct_products_by_rows_in_2).alias("next3_prev3_start_date_span_days"),
        F.last("start_date_span_days", ignorenulls=True).over(w_distinct_products_by_rows_in_3).alias("last_known_start_date_span_days"),
        F.max("start_date_span_days").over(w_distinct_products_by_rows_in_4).alias("start_date_span_days_max_7"),
    )
)

annotated = (
    with_trends
    .withColumn("product_list_text", F.array_join(F.array_sort(F.col("product_list")), ","))
)

result = (
    annotated
    .select(
        "product",
        "start_date_ym",
        "end_date_year",
        "product_list",
        "distinct_products",
        "end_date_cume_dist_to_rows_in_product_end_date_y",
        "start_date_span_days",
        "avg_premium_to_rows_in_product_end_date_year_rat",
        "rn",
        "rows_in_product_distinct_products",
        "prev3_start_date_span_days_avg_2",
        "next3_prev3_start_date_span_days",
        "last_known_start_date_span_days",
        "start_date_span_days_max_7",
        "product_list_text",
        F.concat(F.col("start_date_ym"), F.lit("#"), F.col("rn").cast("string")).alias("start_date_ym_ref"),
        F.abs(F.col("start_date_span_days") - 455).alias("start_date_span_days_distance"),
    )
    .orderBy(F.col("end_date_cume_dist_to_rows_in_product_end_date_y").asc())
)
