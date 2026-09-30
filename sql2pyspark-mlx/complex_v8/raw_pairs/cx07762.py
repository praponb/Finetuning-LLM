from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id = Window.partitionBy("holder_id")
w_all_by_end_date_moving_3 = Window.orderBy(F.col("end_date").desc_nulls_first(), "id").rowsBetween(-3, Window.currentRow)
w_all_by_end_date = Window.orderBy(F.col("end_date").desc_nulls_first(), "id")
w_all_by_end_date_ties = Window.orderBy(F.col("end_date").desc_nulls_first())
w_all_by_end_date_full = Window.orderBy(F.col("end_date").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_latest = Window.partitionBy("status").orderBy(F.col("premium").desc(), F.col("id").desc())

scoped_policys = (
    df_policies
    .filter((~F.col("status").isin("CANCELLED", "LAPSED")) | (F.col("premium") < 3465.4))
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

ranked = (
    scoped_policys
    .select(
        "*",
        F.round(F.col("premium") - F.avg("premium").over(w_holder_id), 2).alias("premium_vs_holder_id_avg"),
        F.sum("premium").over(w_all_by_end_date_moving_3).alias("premium_sum_4"),
        F.lead("start_date", 3).over(w_all_by_end_date).alias("next3_start_date"),
        F.min("premium").over(w_holder_id).alias("holder_id_min_premium"),
        F.dense_rank().over(w_all_by_end_date_ties).alias("end_date_dense_rank"),
        F.first("end_date").over(w_all_by_end_date_full).alias("first_end_date"),
    )
)

tagged = (
    ranked
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("product")), " ", "_")).alias("product_slug"),
    )
)

deduped = (
    tagged
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    deduped
    .select(
        "id",
        F.col("premium_vs_holder_id_avg").alias("premium_vs_holder_id_avg_v"),
        F.col("premium").alias("m_premium"),
    )
    .unpivot(
        ids=["id"],
        values=["premium_vs_holder_id_avg_v", "m_premium"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .orderBy("id", "metric")
)
