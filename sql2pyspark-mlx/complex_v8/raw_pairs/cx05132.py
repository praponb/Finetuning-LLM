from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("product", "holder_id").orderBy(F.col("end_date").desc(), F.col("id").desc())

cleaned = (
    df_policies
    .filter(F.col("holder_id") >= 18)
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

qualified = (
    cleaned.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.status") == "ACTIVE") & (F.col("p.start_date").isNotNull()), "left_anti")
)

deduped = (
    qualified
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

annotated = (
    deduped
    .withColumn("premium_tier", F.when(F.col("premium") >= 3465.4, "HIGH").when(F.col("premium") >= 2289.0, "MEDIUM").otherwise("LOW"))
)

result = (
    annotated
    .groupBy("status", "holder_id")
    .pivot("premium_tier", ["LOW", "MEDIUM"])
    .agg(F.min("premium"))
    .orderBy("status", "holder_id")
)
