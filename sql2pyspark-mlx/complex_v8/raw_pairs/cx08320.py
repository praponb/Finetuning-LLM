from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("sku_id").orderBy(F.col("prior_qty_12h").desc(), F.col("id").desc())

covered = (
    df_stock_moves.alias("a")
    .join(
        df_stock_moves.alias("b"),
        (F.col("b.sku_id") == F.col("a.sku_id"))
        & (F.col("b.moved_at") < F.col("a.moved_at"))
        & (F.col("b.moved_at") >= F.col("a.moved_at") - F.expr("INTERVAL 12 HOURS")),
        "left",
    )
    .groupBy("a.id", "a.sku_id", "a.moved_at")
    .agg(
        F.count("b.id").alias("prior_rows_12h"),
        F.sum("b.qty").alias("prior_qty_12h"),
    )
)

scored = (
    covered
    .filter(F.col("prior_qty_12h").between(250, 1875))
    .select(
        "id",
        "sku_id",
        "prior_qty_12h",
        F.least(F.col("prior_qty_12h"), F.col("prior_rows_12h")).alias("least_prior_qty_12h_prior_rows_12h"),
        F.date_trunc("week", F.col("moved_at")).alias("moved_at_week"),
    )
)

latest = (
    scored
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_parent_sku = (
    latest.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select("s.*", "sk.sku_code")
)

result = (
    with_parent_sku
    .filter(F.col("prior_qty_12h") <= 1375)
    .select(
        "id",
        "prior_qty_12h",
        "least_prior_qty_12h_prior_rows_12h",
        "moved_at_week",
        "sku_code",
        F.date_trunc("month", F.col("moved_at_week")).alias("moved_at_week_month"),
    )
)
