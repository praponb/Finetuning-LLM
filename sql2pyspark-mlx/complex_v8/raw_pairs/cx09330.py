from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_product = Window.partitionBy("holder_id", "product")
w_end_date_month_status_by_end = Window.partitionBy("end_date_month", "status").orderBy(F.col("end_date").desc(), "id")

recent_policys = (
    df_policies
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "status",
        F.month(F.col("end_date")).alias("end_date_month"),
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket"),
    )
)

with_windows = (
    recent_policys
    .select(
        "id",
        "product",
        "start_date",
        "end_date",
        "end_date_month",
        "premium_bucket",
        F.count(F.lit(1)).over(w_holder_id_product).alias("rows_in_holder_id_product"),
        F.row_number().over(w_end_date_month_status_by_end).alias("seq_no"),
        F.sum("end_date_month").over(w_holder_id_product).alias("holder_id_product_sum_end_date_month"),
    )
)

top_rows = (
    with_windows
    .filter(F.col("seq_no") <= 3)
    .drop("seq_no")
)

result = (
    top_rows
    .select(
        "id",
        "holder_id_product_sum_end_date_month",
        "premium_bucket",
        "end_date_month",
        "rows_in_holder_id_product",
    )
    .unpivot(
        ids=["id"],
        values=["holder_id_product_sum_end_date_month", "premium_bucket", "end_date_month", "rows_in_holder_id_product"],
        variableColumnName="field",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("id", "field")
)
