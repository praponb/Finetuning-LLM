from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_start_date_year_by = Window.partitionBy("holder_id", F.year("start_date")).orderBy(F.col("end_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_end_date_year = Window.partitionBy(F.year("end_date"))
w_latest = Window.partitionBy("status").orderBy(F.col("premium").desc(), F.col("id").desc())

recent_policys = (
    df_policies
    .filter((F.col("premium") <= 4678.8) & (F.col("start_date") >= F.lit("2025-01-05").cast("date")))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.quarter(F.col("start_date")).alias("start_date_quarter"),
    )
)

annotated = (
    recent_policys
    .select(
        "*",
        F.upper(F.trim(F.col("status"))).alias("status_clean"),
        F.concat(F.col("status"), F.lit("#"), F.col("id").cast("string")).alias("status_ref"),
    )
)

windowed = (
    annotated
    .withColumn("last_known_start_date", F.last("start_date", ignorenulls=True).over(w_holder_id_start_date_year_by))
    .withColumn("end_date_year_min_premium", F.min("premium").over(w_end_date_year))
)

deduped = (
    windowed
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    deduped
    .rollup("start_date_quarter", "status")
    .agg(
        F.count(F.when(F.col("status").isNotNull(), 1)).alias("n_status_is_not_null"),
        F.count("*").alias("cnt"),
    )
    .orderBy(
        F.col("start_date_quarter").asc_nulls_last(),
        F.col("status").asc_nulls_last(),
    )
)
