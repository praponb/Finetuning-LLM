from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_by_start_date_runnin = Window.partitionBy("product").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_product_by_start_date_moving = Window.partitionBy("product").orderBy("start_date", "id").rowsBetween(-3, Window.currentRow)
w_product_holder_id = Window.partitionBy("product", "holder_id")
w_all_by_days_since_end_date = Window.orderBy("days_since_end_date", "id")
w_holder_id_product = Window.partitionBy("holder_id", "product")
w_all_by_days_since_end_date_r = Window.orderBy("days_since_end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_policys = (
    df_policies
    .filter(F.col("product").isin("TRAVEL", "LIFE"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("end_date")).alias("days_since_end_date"),
    )
)

sequenced = (
    scoped_policys
    .select(
        "*",
        F.sum("days_since_end_date").over(w_product_by_start_date_runnin).alias("running_days_since_end_date"),
        F.round(F.avg("days_since_end_date").over(w_product_by_start_date_moving), 2).alias("days_since_end_date_avg_4"),
        F.count(F.lit(1)).over(w_product_holder_id).alias("rows_in_product_holder_id"),
    )
)

with_windows = (
    sequenced
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "days_since_end_date",
        "running_days_since_end_date",
        "rows_in_product_holder_id",
        F.lag("end_date").over(w_all_by_days_since_end_date).alias("prev_end_date"),
        F.count(F.lit(1)).over(w_holder_id_product).alias("rows_in_holder_id_product"),
        F.ntile(3).over(w_all_by_days_since_end_date).alias("days_since_end_date_tercile"),
        F.first("days_since_end_date_avg_4", ignorenulls=True).over(w_all_by_days_since_end_date_r).alias("first_known_days_since_end_date_avg_4"),
    )
)

result = (
    with_windows
    .rollup("days_since_end_date_tercile", "holder_id", "product")
    .agg(
        F.grouping("days_since_end_date_tercile").alias("is_total_days_since_end_date_tercile"),
        F.count("*").alias("n_rows"),
    )
    .orderBy(
        F.col("days_since_end_date_tercile").asc_nulls_last(),
        F.col("holder_id").asc_nulls_last(),
        F.col("product").asc_nulls_last(),
    )
)
