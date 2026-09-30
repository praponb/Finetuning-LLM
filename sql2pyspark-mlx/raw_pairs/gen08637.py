from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.orderBy(F.col("started_at").desc(), "id")
w_ordered_2 = Window.partitionBy("device", "customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("customer_id", "device").orderBy(F.col("started_at").desc(), "id")
w4 = Window.partitionBy("customer_id", "device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy(F.month("started_at"), "customer_id").orderBy(F.col("started_at").desc(), "id")

metrics = (
    df_sessions
    .filter(F.col("customer_id").isin([23, 38]))
    .withColumn("seq_no", F.row_number().over(w_ordered))
    .withColumn("first_known_browser", F.first("browser", ignorenulls=True).over(w_ordered_2))
    .withColumn("row_seq", F.row_number().over(w_grp))
    .withColumn("first_known_country", F.first("country", ignorenulls=True).over(w4))
    .withColumn("row_seq_2", F.row_number().over(w_recent))
)

remaining_keys = (
    metrics
    .select("device", "customer_id")
    .exceptAll(
        df_sessions
        .filter(F.col("country") == "JP")
        .select("device", "customer_id")
    )
)

kept_rows = (
    metrics
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.device") == F.col("k.device")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

with_dims = (
    kept_rows
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.name"), F.col("p.segment"), F.col("p.credit_limit"))
)

with_dims_sessions = (
    with_dims
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id").eqNullSafe(F.col("d.id")), "left")
    .select(
        "e.id",
        "e.customer_id",
        "e.browser",
        "e.started_at",
        "e.seq_no",
        "e.row_seq",
        "e.name",
        "e.segment",
        "e.credit_limit",
        F.col("d.age"),
        F.col("d.city"),
    )
)

result = (
    with_dims_sessions
    .select(
        "id",
        "customer_id",
        "browser",
        "seq_no",
        "row_seq",
        "segment",
        F.when(F.col("segment").rlike("^(Consumer|SMB)$"), "EXTENDED").when(F.col("segment").rlike("^(Corporate)$"), "GROUP_A").otherwise("CORE").alias("segment_zone"),
    )
)
