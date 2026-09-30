from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_by_start_date_ties = Window.partitionBy("holder_id").orderBy(F.col("start_date").desc())
w_all_by_start_date_ties = Window.orderBy("start_date")
w_all_by_start_date_full = Window.orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped_policys = (
    df_policies
    .filter((F.col("end_date") < F.lit("2026-08-31").cast("date")) & (F.col("premium") < 3817.5))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "status",
    )
)

metrics = (
    scoped_policys
    .select(
        "*",
        F.round(F.cume_dist().over(w_holder_id_by_start_date_ties), 3).alias("start_date_cume_dist"),
        F.round(F.cume_dist().over(w_all_by_start_date_ties), 3).alias("start_date_cume_dist_2"),
        F.nth_value("start_date", 2).over(w_all_by_start_date_full).alias("second_start_date"),
    )
)

enriched = (
    metrics.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select("s.*", "p.holder_name")
)

result = (
    enriched
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "status",
        "start_date_cume_dist_2",
        "second_start_date",
        F.when(F.col("start_date_cume_dist") >= 0.8, "HIGH").when(F.col("start_date_cume_dist") >= 0.4, "MEDIUM").otherwise("LOW").alias("start_date_cume_dist_tier"),
    )
    .orderBy(F.col("status").asc(), F.col("start_date_cume_dist_tier").asc())
)
