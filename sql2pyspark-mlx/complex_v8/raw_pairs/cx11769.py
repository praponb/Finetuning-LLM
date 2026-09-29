from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("country", "plan").orderBy("birth_year", "id")

valid = (
    df_listeners
    .select(
        "id",
        "plan",
        "country",
        "birth_year",
        F.size(F.col("interests")).alias("interests_count"),
    )
)

unique_rows = (
    valid
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

qualified = (
    unique_rows.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.id")) & (F.col("p.ms_played") >= 62294), "left_anti")
)

result = (
    qualified
    .select(
        "id",
        "plan",
        "country",
        "birth_year",
        "interests_count",
    )
    .orderBy(F.col("country").asc(), "birth_year", "id")
    .limit(10)
)
