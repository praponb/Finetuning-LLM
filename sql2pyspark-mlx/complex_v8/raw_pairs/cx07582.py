from pyspark.sql import functions as F

valid = (
    df_adjusters
    .filter((F.length(F.trim(F.col("adjuster_name"))) >= 4) & (F.col("level") <= 3))
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.upper(F.trim(F.col("adjuster_name"))).alias("adjuster_name_clean"),
        F.when(F.col("level") >= 3, "A").when(F.col("level") >= 2, "B").otherwise("C").alias("level_tier"),
        F.concat_ws("-", F.col("team"), F.col("adjuster_name")).alias("team_adjuster_name_label"),
    )
)

scored = (
    valid
    .select(
        "id",
        "adjuster_name",
        "level_tier",
        F.length(F.trim(F.col("adjuster_name_clean"))).alias("adjuster_name_clean_len"),
        F.length(F.trim(F.col("team_adjuster_name_label"))).alias("team_adjuster_name_label_len"),
    )
)

derived = (
    scored
    .select(
        "*",
        F.initcap(F.trim(F.col("adjuster_name"))).alias("adjuster_name_clean"),
        F.lower(F.regexp_replace(F.trim(F.col("level_tier")), " ", "_")).alias("level_tier_slug"),
    )
)

result = (
    derived
    .select(
        "id",
        F.col("adjuster_name_clean_len").alias("m_adjuster_name_clean_len"),
        F.col("team_adjuster_name_label_len").alias("team_adjuster_name_label_len_v"),
    )
    .unpivot(
        ids=["id"],
        values=["m_adjuster_name_clean_len", "team_adjuster_name_label_len_v"],
        variableColumnName="field",
        valueColumnName="value",
    )
    .orderBy("id", "field")
)
