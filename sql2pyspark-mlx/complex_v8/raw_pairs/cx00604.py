from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_end_date_year_product_by_pre = Window.partitionBy(F.year("end_date"), "product").orderBy("premium", "id")
w_end_date_year_product_by_pre_2 = Window.partitionBy(F.year("end_date"), "product").orderBy("premium", "id").rowsBetween(-2, Window.currentRow)
w_holder_id_product = Window.partitionBy("holder_id", "product")
w_end_date_year_product_by_pre_3 = Window.partitionBy(F.year("end_date"), "product").orderBy("premium", "id").rowsBetween(-6, Window.currentRow)

prepared_policys = (
    df_policies
    .filter(~F.col("status").isin("EXPIRED", "CANCELLED"))
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

with_windows = (
    prepared_policys
    .select(
        "id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.datediff(F.col("end_date"), F.lag("end_date").over(w_end_date_year_product_by_pre)).alias("days_since_prev_end_date"),
        F.round(F.avg("premium").over(w_end_date_year_product_by_pre_2), 2).alias("premium_avg_3"),
        F.count(F.lit(1)).over(w_holder_id_product).alias("rows_in_holder_id_product"),
        F.round(F.avg("premium").over(w_end_date_year_product_by_pre_3), 2).alias("premium_avg_7"),
    )
)

result = (
    with_windows
    .select("id", "premium_avg_3", "premium_avg_7")
    .unpivot(
        ids=["id"],
        values=["premium_avg_3", "premium_avg_7"],
        variableColumnName="measure",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
