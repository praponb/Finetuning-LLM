from pyspark.sql import functions as F

artist_side = (
    df_artists
    .filter(F.upper(F.trim(F.col("artist_name"))).like("N%"))
    .select(
        F.col("country").alias("name"),
        F.col("debut_year").alias("qty"),
        F.lit("artist").alias("record_type"),
    )
)

listener_side = (
    df_listeners
    .filter((F.col("country").isin("BR", "TH", "KR")) & (F.col("plan") == "premium"))
    .select("plan", "id", F.lit("listener").alias("record_type"))
)

listener_rows = (
    df_listeners
    .filter(F.col("signup_date") >= F.lit("2024-10-07").cast("date"))
    .select("username", "id", F.lit("listener").alias("record_type"))
)

all_events = (
    artist_side
    .union(listener_side)
    .union(listener_rows)
)

result = (
    all_events
    .groupBy("record_type")
    .agg(F.count("*").alias("n_rows"), F.sum("qty").alias("total_qty"))
    .orderBy("record_type")
)
