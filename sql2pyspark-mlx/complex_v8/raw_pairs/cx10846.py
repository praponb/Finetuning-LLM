from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_team_adjuster_name_len_by_ad = Window.partitionBy("team", "adjuster_name_len").orderBy("adjuster_name_len", "id")
w_team_adjuster_name_len_by_ad_2 = Window.partitionBy("team", "adjuster_name_len").orderBy("adjuster_name_len", "id").rowsBetween(-1, -1)

valid_adjusters = (
    df_adjusters
    .filter((F.col("level") < 3) | (~F.col("team").isin("Health", "Property")))
)

tagged = (
    valid_adjusters
    .filter(F.upper(F.trim(F.col("adjuster_name"))).like("H%"))
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("adjuster_name")), " ", "_")).alias("adjuster_name_slug"),
        F.length(F.trim(F.col("adjuster_name"))).alias("adjuster_name_len"),
        F.concat_ws("-", F.col("adjuster_name"), F.col("team")).alias("adjuster_name_team_label"),
    )
)

ranked = (
    tagged
    .select(
        "*",
        F.lag("level", 2).over(w_team_adjuster_name_len_by_ad).alias("prev2_level"),
        F.max("adjuster_name_len").over(w_team_adjuster_name_len_by_ad_2).alias("adjuster_name_len_max_1_prior"),
    )
)

screened = (
    ranked.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.id")) & (F.col("c.approved_amount").between(0.0, 14993.0)), "left_semi")
)

result = (
    screened
    .select(
        "id",
        "adjuster_name_len",
        "prev2_level",
        "level",
    )
    .unpivot(
        ids=["id"],
        values=["adjuster_name_len", "prev2_level", "level"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("id", "metric_name")
)
