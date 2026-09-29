from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host", "attr_key").orderBy("log_ts", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.orderBy("log_ts", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.date_trunc("month", "log_ts"), "attr_key").orderBy(F.col("log_ts").desc(), "id", "attr_key")

lines = (
    df_app_logs
    .select("id", "host", "log_ts", F.explode("context").alias("attr_key", "value"))
)

result = (
    lines
    .filter(F.col("attr_key") == "region")
    .select(
        "*",
        F.last("attr_key", ignorenulls=True).over(w).alias("last_known_attr_key"),
        F.first("attr_key", ignorenulls=True).over(w_grp).alias("first_known_attr_key"),
        F.last("attr_key", ignorenulls=True).over(w_grp).alias("last_known_attr_key_2"),
        F.row_number().over(w3).alias("seq_no"),
    )
)
