from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_csat_agent_id_by_requester_i = Window.partitionBy("csat", "agent_id").orderBy(F.col("requester_id").desc())
w_agent_id_csat_by_csat_ties = Window.partitionBy("agent_id", "csat").orderBy("csat")
w_agent_id_csat_by_requester_i = Window.partitionBy("agent_id", "csat").orderBy("requester_id", "id")
w_csat_agent_id_by_requester_i_2 = Window.partitionBy("csat", "agent_id").orderBy(F.col("requester_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_csat_by_csat_running = Window.partitionBy("csat").orderBy("csat", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_csat_by_csat = Window.partitionBy("csat").orderBy("csat", "id")
w_agent_id_csat = Window.partitionBy("agent_id", "csat")
w_csat_agent_id = Window.partitionBy("csat", "agent_id")

filtered_tickets = (
    df_tickets
    .filter(F.array_contains(F.col("tags"), "upgrade"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "csat",
        "tags",
    )
)

windowed = (
    filtered_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        F.round(F.percent_rank().over(w_csat_agent_id_by_requester_i), 3).alias("requester_id_percent_rank"),
        F.dense_rank().over(w_agent_id_csat_by_csat_ties).alias("csat_dense_rank"),
        F.lag("csat", 1, 0).over(w_agent_id_csat_by_requester_i).alias("prev_csat"),
        F.first("csat").over(w_csat_agent_id_by_requester_i_2).alias("first_csat"),
        F.max("requester_id").over(w_csat_by_csat_running).alias("max_requester_id_so_far"),
        F.lead("requester_id").over(w_csat_by_csat).alias("next_requester_id"),
        F.max("csat").over(w_agent_id_csat).alias("agent_id_csat_max_csat"),
        F.max("csat").over(w_csat_agent_id).alias("csat_agent_id_max_csat"),
        F.count(F.lit(1)).over(w_agent_id_csat).alias("rows_in_agent_id_csat"),
    )
)

result = (
    windowed
    .select(
        "id",
        "requester_id",
        "requester_id_percent_rank",
        "prev_csat",
        "first_csat",
        "max_requester_id_so_far",
        "next_requester_id",
        "agent_id_csat_max_csat",
        "rows_in_agent_id_csat",
        (F.col("max_requester_id_so_far") + F.col("csat_agent_id_max_csat")).alias("max_requester_id_so_far_plus_csat_agent_id_max_c"),
    )
    .orderBy(
        F.col("max_requester_id_so_far_plus_csat_agent_id_max_c").desc_nulls_first(),
        F.col("prev_csat").desc_nulls_first(),
    )
)
