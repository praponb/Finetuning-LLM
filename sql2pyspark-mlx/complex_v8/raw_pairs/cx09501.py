from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_greatest_csat_request = Window.orderBy(F.col("greatest_csat_requester_id").desc(), "id")
w_all_by_greatest_csat_request_2 = Window.orderBy(F.col("greatest_csat_requester_id").desc())
w_csat_agent_id = Window.partitionBy("csat", "agent_id")

recent_tickets = (
    df_tickets
    .filter((F.col("requester_id") >= 7) & (F.col("csat") >= 2))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "csat",
        F.greatest(F.col("csat"), F.col("requester_id")).alias("greatest_csat_requester_id"),
    )
)

sequenced = (
    recent_tickets
    .select(
        "*",
        F.ntile(4).over(w_all_by_greatest_csat_request).alias("greatest_csat_requester_id_quartile"),
        F.dense_rank().over(w_all_by_greatest_csat_request_2).alias("greatest_csat_requester_id_dense_rank"),
        F.count(F.lit(1)).over(w_csat_agent_id).alias("rows_in_csat_agent_id"),
        F.lag("csat", 3).over(w_all_by_greatest_csat_request).alias("prev3_csat"),
        F.rank().over(w_all_by_greatest_csat_request_2).alias("greatest_csat_requester_id_rank"),
    )
)

top_rows = (
    sequenced
    .filter(F.col("greatest_csat_requester_id_dense_rank") <= 5)
    .drop("greatest_csat_requester_id_dense_rank")
)

result = (
    top_rows
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "csat",
        "greatest_csat_requester_id_quartile",
        "prev3_csat",
        "greatest_csat_requester_id_rank",
        F.least(F.col("requester_id"), F.col("prev3_csat")).alias("least_requester_id_prev3_csat"),
    )
    .orderBy(F.col("agent_id").asc(), "least_requester_id_prev3_csat")
)
