from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_by_premium_filled_ti = Window.partitionBy("product").orderBy("premium_filled")
w_end_date_year_product = Window.partitionBy(F.year("end_date"), "product")
w_product_holder_id_by_rows_in = Window.partitionBy("product", "holder_id").orderBy("rows_in_end_date_year_product", "id")
w_product_holder_id_by_end_dat = Window.partitionBy("product", "holder_id").orderBy(F.col("end_date").asc_nulls_last(), "id")
w_product_holder_id_by_rows_in_2 = Window.partitionBy("product", "holder_id").orderBy("rows_in_end_date_year_product", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_product = Window.partitionBy("product")

scoped_policys = (
    df_policies
    .filter(F.col("status") == "LAPSED")
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
    )
)

sequenced = (
    scoped_policys
    .select(
        "*",
        F.dense_rank().over(w_product_by_premium_filled_ti).alias("premium_filled_dense_rank"),
        F.count(F.lit(1)).over(w_end_date_year_product).alias("rows_in_end_date_year_product"),
    )
)

top_per_group = (
    sequenced
    .filter((F.col("premium_filled_dense_rank") == 1) & (F.col("premium_filled") < 3560.0))
)

screened = (
    top_per_group
    .crossJoin(top_per_group.agg(F.avg("rows_in_end_date_year_product").alias("overall_avg_rows_in_end_date_year_product")))
    .filter(F.col("rows_in_end_date_year_product") > F.col("overall_avg_rows_in_end_date_year_product"))
    .drop("overall_avg_rows_in_end_date_year_product")
)

windowed = (
    screened
    .select(
        "*",
        F.rank().over(w_product_holder_id_by_rows_in).alias("rows_in_end_date_year_product_rank"),
        (F.col("premium_filled_dense_rank") - F.lag("premium_filled_dense_rank").over(w_product_holder_id_by_end_dat)).alias("premium_filled_dense_rank_change"),
        F.last("end_date", ignorenulls=True).over(w_product_holder_id_by_rows_in_2).alias("last_known_end_date"),
        F.size(F.collect_set("holder_id").over(w_product)).alias("distinct_holder_id_in_product"),
        F.round(F.col("premium_filled") - F.avg("premium_filled").over(w_product), 2).alias("premium_filled_vs_product_avg"),
    )
)

result = (
    windowed
    .filter(F.col("premium_filled_vs_product_avg").between(-3000.0, 3000.0))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "premium_filled",
        "rows_in_end_date_year_product",
        "rows_in_end_date_year_product_rank",
        "premium_filled_dense_rank_change",
        "premium_filled_vs_product_avg",
        F.round(F.col("premium_filled") / F.nullif(F.col("premium"), F.lit(0)), 3).alias("premium_filled_per_premium"),
    )
    .orderBy(F.col("premium_filled_dense_rank_change").asc_nulls_last())
)
