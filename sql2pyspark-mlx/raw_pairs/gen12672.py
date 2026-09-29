from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

with_latest = (
    df_stores
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.status"))
)

result = (
    with_latest
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        "status",
        F.when(F.col("store_name").rlike("^(Downtown Express|Old Town)$"), "GROUP_A").otherwise("GROUP_B").alias("store_name_zone"),
    )
)
